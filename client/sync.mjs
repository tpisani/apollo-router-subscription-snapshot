export class Synchronizer {
  constructor(api, scopeId) { this.api = api; this.scopeId = scopeId; this.orders = new Map(); this.cursor = 0; }
  initialize(snapshot) {
    if (snapshot.scopeId !== this.scopeId) throw new Error('Snapshot de outro escopo');
    this.orders = new Map(snapshot.orders.map(order => [order.id,{...order}]));
    this.cursor = Number(snapshot.cursor);
  }
  apply(change) {
    if (change.scopeId !== this.scopeId) throw new Error('Evento de outro escopo');
    const seq = Number(change.cursor);
    if (seq <= this.cursor) return; // Reentrega/overlap do replay com live.
    // Neste demo, a sequência é contígua POR ESCOPO e não há filtros adicionais.
    if (seq !== this.cursor + 1) throw new Error(`Lacuna: ${this.cursor} -> ${seq}`);
    if (change.operation === 'DELETE') this.orders.delete(change.order.id);
    else this.orders.set(change.order.id,{...change.order});
    this.cursor = seq;
  }
  async catchUp(target) {
    let h = Number(target);
    try {
      while (this.cursor < h) {
        const page = await this.api.changes(String(this.cursor),String(h));
        if (!page.events.length) throw new Error('Histórico incompleto; cursor não avançou');
        for (const event of page.events) this.apply(event);
        if (Number(page.nextCursor) !== this.cursor) throw new Error('Página inconsistente');
      }
    } catch (error) {
      if (error.code !== 'CURSOR_EXPIRED') throw error;
      // fresh=true tem chave diferente no Router e ignora o cache interno.
      this.initialize(await this.api.snapshot(true));
      h = Number(await this.api.head());
      await this.catchUp(h);
    }
  }
  async accept(frame) {
    if (frame.scopeId !== this.scopeId) throw new Error('Frame de outro escopo');
    const target = Number(frame.cursor);
    if (target <= this.cursor) return;
    if (frame.kind === 'CHANGE' && target === this.cursor + 1) {
      if (!frame.change || frame.change.cursor !== frame.cursor) throw new Error('Frame inconsistente');
      this.apply(frame.change);
    } else {
      // Checkpoint detecta perdas; evento fora de ordem provoca recuperação pelo log.
      await this.catchUp(target);
    }
  }
  view() { return { cursor: String(this.cursor), orders: [...this.orders.values()].sort((a,b) => a.id.localeCompare(b.id)) }; }
}

export async function runSession(sync, frames, { resume = false, onUpdate = () => {} } = {}) {
  const iterator = frames[Symbol.asyncIterator]();
  try {
    const first = await iterator.next();
    if (first.done) throw new Error('Subscription terminou antes do primeiro frame');
    // Um frame recebido confirma participação no fluxo compartilhado. O transporte
    // preserva os frames seguintes enquanto as queries de bootstrap executam.
    if (!resume) sync.initialize(await sync.api.snapshot(false));
    await sync.catchUp(await sync.api.head());
    await sync.accept(first.value); onUpdate(sync.view());
    while (true) {
      const next = await iterator.next();
      if (next.done) throw new Error('Subscription encerrada; reconectar e recuperar');
      await sync.accept(next.value); onUpdate(sync.view());
    }
  } finally { await iterator.return?.(); }
}
