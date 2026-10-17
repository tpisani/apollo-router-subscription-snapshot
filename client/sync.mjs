import { applyPatch } from '../src/patch.mjs';

// Base comum: estado + versão de um canal de uma partida. A base provisória vem
// da chave compartilhada (minVersion 0, provavelmente do cache); o "agora" vem
// sempre do stream. synced = já recebeu um frame nesta conexão e está nele.
class ChannelSync {
  constructor({ matchId, api }) {
    this.matchId = matchId; this.api = api;
    this.state = null; this.version = null; this.synced = false; this.base = null; this.busy = false;
    this.stats = { snapshots: 0, reseeds: 0, frames: 0, applied: 0, ignored: 0 };
  }
  initialize(snapshot) {
    if (snapshot.matchId !== this.matchId) throw new Error('Snapshot de outra partida');
    if (this.version !== null && snapshot.version <= this.version) return;
    this.state = this.fromSnapshot(snapshot.state); this.version = snapshot.version; this.stats.snapshots++;
  }
  fromSnapshot(state) { return structuredClone(state); }
  startBase() {
    this.base ??= this.api.snapshot(0).catch(error => { this.base = null; throw error; });
    return this.base;
  }
  // Snapshot com minVersion: chave nova, e o servidor garante versão >= min.
  async reseed(min) {
    const snapshot = await this.api.snapshot(min);
    if (snapshot.version < min) throw new Error(`Snapshot violou minVersion: ${snapshot.version} < ${min}`);
    this.initialize(snapshot); this.stats.reseeds++;
  }
  async accept(frame) {
    if (frame.matchId !== this.matchId) throw new Error('Frame de outra partida');
    this.busy = true; this.stats.frames++;
    try { await this.handle(frame); this.synced = true; } finally { this.busy = false; }
  }
  view() { return { version: this.version, synced: this.synced, state: this.state }; }
}

// Placar: cada frame traz o estado inteiro. O mais novo vence; não existe lacuna
// a recuperar, e um late joiner fica certo no primeiro frame, sem query.
export class StateSync extends ChannelSync {
  async handle(frame) {
    if (this.version !== null && frame.version <= this.version) { this.stats.ignored++; return; }
    this.state = structuredClone(frame.state); this.version = frame.version; this.stats.applied++;
  }
}

// Estatísticas: DELTA com valores finais, aplicável quando from <= versão local
// < version (sobreposição é segura, ver src/patch.mjs); STATE substitui tudo, como
// no placar. Se um DELTA começa depois da versão local (ou um CHECKPOINT está
// acima dela), faltou algo: um snapshot com minVersion resolve.
export class DeltaSync extends ChannelSync {
  fromSnapshot(state) {
    const { players, ...teams } = structuredClone(state);
    return { ...teams, players: Object.fromEntries(players.map(({ id, ...p }) => [id, p])) };
  }
  async handle(frame) {
    if (frame.kind === 'STATE') {
      if (this.version !== null && frame.version <= this.version) { this.stats.ignored++; return; }
      this.state = this.fromSnapshot(frame.state); this.version = frame.version; this.stats.applied++;
      return;
    }
    if (this.version === null) {
      const snapshot = await this.startBase();
      if (this.version === null) this.initialize(snapshot);
    }
    if (frame.version <= this.version) { this.stats.ignored++; return; }   // já tenho isso
    if (frame.kind === 'DELTA' && frame.from <= this.version) {
      applyPatch(this.state, frame.patch); this.version = frame.version; this.stats.applied++;
      return;
    }
    await this.reseed(frame.version); // DELTA adiantado ou CHECKPOINT acima: perdi algo
  }
}

// Uma conexão do stream; frames processados em ordem.
export async function runSession(sync, frames, { onUpdate = () => {} } = {}) {
  sync.synced = false;
  const iterator = frames[Symbol.asyncIterator]();
  if (sync.version === null) {
    // Render provisório: a base pode chegar antes do primeiro frame.
    sync.startBase().then(snapshot => {
      if (sync.version === null) { sync.initialize(snapshot); onUpdate(sync.view()); }
    }, () => {});
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

// Reconexão com backoff e watchdog (sem frame em stallMs = conexão morta). O
// estado é mantido; a retomada é o próximo frame (placar) ou um snapshot (stats).
export async function runSync({ sync, connect, signal, stallMs = 5000, onUpdate = () => {}, onError = () => {}, wait = backoff }) {
  let attempt = 0;
  while (!signal?.aborted) {
    const session = new AbortController();
    const abort = () => session.abort();
    signal?.addEventListener('abort', abort, { once: true });
    let lastFrame = Date.now();
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
