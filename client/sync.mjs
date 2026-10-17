// Estado + versão de um canal de uma partida. Os dois canais (placar e
// estatísticas) mandam frames STATE com o estado inteiro, então a regra é uma só:
// fica o de maior versão. Não há lacuna a recuperar: perda, duplicata, reconexão
// e late joiner se resolvem com o próximo frame, retido pelo edge/Router ou periódico.
// A base (minVersion 0, provavelmente do cache) só serve para desenhar a tela antes
// do primeiro frame. synced = já recebeu um frame nesta conexão.
//   fromWire: converte o formato da API (ex.: jogadores em lista → mapa por id).
export class StateSync {
  constructor({ matchId, api, fromWire = state => state }) {
    this.matchId = matchId; this.api = api; this.fromWire = fromWire;
    this.state = null; this.version = null; this.synced = false; this.base = null; this.busy = false;
    this.stats = { snapshots: 0, frames: 0, applied: 0, ignored: 0, stale: 0 };
  }
  initialize(snapshot) {
    if (snapshot.matchId !== this.matchId) throw new Error('Snapshot de outra partida');
    if (this.version !== null && snapshot.version <= this.version) return;
    this.state = this.fromWire(structuredClone(snapshot.state)); this.version = snapshot.version; this.stats.snapshots++;
  }
  startBase() {
    this.base ??= this.api.snapshot(0).catch(error => { this.base = null; throw error; });
    return this.base;
  }
  async accept(frame) {
    if (frame.matchId !== this.matchId) throw new Error('Frame de outra partida');
    this.stats.frames++;
    if (this.version !== null && frame.version <= this.version) {
      // Mais velho ou igual ao que já tenho: frame retido de outra instância, periódico
      // atrasado ou duplicata. Nunca regride.
      this.stats.ignored++; if (frame.version < this.version) this.stats.stale++;
    } else {
      this.state = this.fromWire(structuredClone(frame.state)); this.version = frame.version; this.stats.applied++;
    }
    this.synced = true;
  }
  view() { return { version: this.version, synced: this.synced, state: this.state }; }
}

export const statsFromWire = ({ players, ...teams }) => ({ ...teams, players: Object.fromEntries(players.map(({ id, ...p }) => [id, p])) });
export const createSync = (channel, options) => new StateSync({ ...options, fromWire: channel === 'stats' ? statsFromWire : undefined });

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
// estado é mantido; a retomada é o próximo frame (retido pelo edge/Router ou periódico).
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
