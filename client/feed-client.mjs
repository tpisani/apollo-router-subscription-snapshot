import { setTimeout as delay } from 'node:timers/promises';

export class GapError extends Error {}

// Estado local de um escopo. Só avança por frames contíguos; qualquer
// inconsistência derruba a sessão e a reconexão retoma do último cursor válido.
export class FeedState {
  constructor(scopeId) { this.scopeId = scopeId; this.orders = new Map(); this.cursor = null; }

  apply(frame) {
    if (frame.scopeId !== this.scopeId) throw new Error('Frame de outro escopo');
    if (frame.kind === 'SNAPSHOT') {
      this.orders = new Map(frame.orders.map(order => [order.id, { ...order }]));
      this.cursor = Number(frame.cursor);
      return;
    }
    if (this.cursor === null) throw new GapError('CHANGES antes de qualquer SNAPSHOT');
    for (const change of frame.changes) {
      const seq = Number(change.cursor);
      if (seq <= this.cursor) continue; // Reentrega após reconexão.
      if (seq !== this.cursor + 1) throw new GapError(`Lacuna: ${this.cursor} -> ${seq}`);
      if (change.operation === 'DELETE') this.orders.delete(change.order.id);
      else this.orders.set(change.order.id, { ...change.order });
      this.cursor = seq;
    }
    if (this.cursor < Number(frame.cursor)) throw new GapError(`Frame incompleto: ${this.cursor} < ${frame.cursor}`);
  }

  view() {
    return { cursor: this.cursor === null ? null : String(this.cursor), orders: [...this.orders.values()].sort((a, b) => a.id.localeCompare(b.id)) };
  }
}

const backoff = attempt => Math.min(10_000, 200 * 2 ** attempt) * (0.5 + Math.random() / 2);

// Loop de sessão. subscribe({ scopeId, after }) devolve um AsyncIterable de frames
// e é chamado de novo a cada reconexão com o cursor atual, então a retomada
// custa um único round trip: o servidor envia só o que falta.
export async function runFeed({ state, subscribe, signal, onFrame = () => {}, onError = () => {}, wait = backoff }) {
  let attempt = 0;
  while (!signal?.aborted) {
    try {
      const after = state.cursor === null ? null : String(state.cursor);
      for await (const frame of subscribe({ scopeId: state.scopeId, after })) {
        state.apply(frame); attempt = 0; onFrame(frame, state);
      }
      if (signal?.aborted) break;
      throw new Error('Feed encerrado pelo servidor');
    } catch (error) {
      if (signal?.aborted) break;
      onError(error, attempt);
      await delay(wait(attempt++), undefined, { signal }).catch(() => {});
    }
  }
}
