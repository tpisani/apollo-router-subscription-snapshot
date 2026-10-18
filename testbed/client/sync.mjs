// State + version of one channel of a match. Both channels (scoreboard and
// stats) send STATE frames with the full state, so there is a single rule:
// the highest version wins. There is no gap to recover: loss, duplicates, reconnects
// and late joiners are solved by the next frame, retained by the edge/Router or periodic.
// The base (minVersion 0, probably from the cache) only serves to draw the screen
// before the first frame. synced = already got a frame on this connection.
//   fromWire: converts the API format (e.g. players as a list → map by id).
export class StateSync {
  constructor({ matchId, api, fromWire = state => state }) {
    this.matchId = matchId; this.api = api; this.fromWire = fromWire;
    this.state = null; this.version = null; this.synced = false; this.base = null; this.busy = false;
    this.stats = { snapshots: 0, frames: 0, applied: 0, ignored: 0, stale: 0 };
  }
  initialize(snapshot) {
    if (snapshot.matchId !== this.matchId) throw new Error('Snapshot from another match');
    if (this.version !== null && snapshot.version <= this.version) return;
    this.state = this.fromWire(structuredClone(snapshot.state)); this.version = snapshot.version; this.stats.snapshots++;
  }
  startBase() {
    this.base ??= this.api.snapshot(0).catch(error => { this.base = null; throw error; });
    return this.base;
  }
  async accept(frame) {
    if (frame.matchId !== this.matchId) throw new Error('Frame from another match');
    this.stats.frames++;
    if (this.version !== null && frame.version <= this.version) {
      // Older than or equal to what I have: a retained frame from another instance, a
      // late periodic frame or a duplicate. Never goes back.
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
// The scoreboard at FULL_TIME is final: the subgraph ends the stream and the client does not reconnect.
export const isFinalScoreboard = state => state?.status === 'FULL_TIME';

// One stream connection; frames processed in order. Returns 'final' when the
// state is final (isFinal); otherwise the end of the stream is a failure and the caller reconnects.
export async function runSession(sync, frames, { onUpdate = () => {}, isFinal = () => false } = {}) {
  sync.synced = false;
  const iterator = frames[Symbol.asyncIterator]();
  if (sync.version === null) {
    // Provisional render: the base may arrive before the first frame.
    sync.startBase().then(snapshot => {
      if (sync.version === null) { sync.initialize(snapshot); onUpdate(sync.view()); }
    }, () => {});
  }
  try {
    while (true) {
      const next = await iterator.next();
      if (next.done) {
        if (isFinal(sync.state)) return 'final';
        throw new Error('Stream ended; reconnect');
      }
      await sync.accept(next.value);
      onUpdate(sync.view());
      if (isFinal(sync.state)) return 'final';
    }
  } finally { await iterator.return?.(); }
}

const backoff = attempt => Math.min(10_000, 200 * 2 ** attempt) * (0.5 + Math.random() / 2);

// Reconnect with backoff and a watchdog (no frame within stallMs = dead connection). The
// state is kept; resuming is the next frame (retained by the edge/Router or periodic).
// Ends when the state becomes final (isFinal) or on signal.
export async function runSync({ sync, connect, signal, stallMs = 5000, onUpdate = () => {}, onError = () => {}, wait = backoff, isFinal = () => false }) {
  let attempt = 0;
  while (!signal?.aborted) {
    const session = new AbortController();
    const abort = () => session.abort();
    signal?.addEventListener('abort', abort, { once: true });
    let lastFrame = Date.now();
    const watchdog = setInterval(() => {
      if (sync.busy) lastFrame = Date.now();
      else if (Date.now() - lastFrame > stallMs) session.abort(new Error('Stream stalled'
));
    }, Math.min(1000, stallMs / 2));
    try {
      const outcome = await runSession(sync, connect(session.signal), { isFinal, onUpdate: view => { lastFrame = Date.now(); attempt = 0; onUpdate(view); } });
      if (outcome === 'final') break;
    } catch (error) {
      if (signal?.aborted) break;
      onError(session.signal.aborted && session.signal.reason instanceof Error ? session.signal.reason : error, attempt);
      await new Promise(resolve => setTimeout(resolve, wait(attempt++)));
    } finally {
      clearInterval(watchdog); session.abort(); signal?.removeEventListener('abort', abort);
    }
  }
}
