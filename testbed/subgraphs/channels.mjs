import { mergePatches } from './patch.mjs';

// A channel = one versioned document per match + a sending policy.
//
// mode 'state' (used by both channels: scoreboard and stats)
//   STATE { version, state }: the full state. The newest replaces the previous ones,
//   so loss, duplicates and late joiners are solved by the next frame, without a query,
//   and the last frame can be retained and resent by whoever sits in between (edge/Router).
//   throttleMs = 0: one frame per commit, right away (scoreboard).
//   throttleMs > 0: limiter. If nothing went out in the last throttleMs, the frame goes
//   out right away; otherwise it goes out at the end of the window with the latest state
//   (stats: at most 2 frames/s in a burst, millisecond latency in a quiet game).
//   Every periodicMs a STATE goes out even without changes: a liveness signal and the
//   state for whoever joined an already open upstream.
//
// mode 'delta' (only in the comparison experiments, scripts/experiments.mjs)
//   DELTA { from, version, patch } per coalesceMs window (or STATE, if smaller) and a
//   periodic CHECKPOINT { version }. Kept on record as a measured alternative.
//
// Frames are the same for every subscriber of the match: the Router deduplicates.
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
    // Only what the stream already delivered, never the head: a commit still in the
    // window must not make the client think it missed something.
    return { matchId: key, kind: 'CHECKPOINT', version: entry.delivered, from: null, patch: null, committedAt: null };
  }

  publish(change) {
    const entry = this.keys.get(change.key);
    if (!entry) return;
    entry.latest = { version: change.version, state: change.state };
    if (this.mode === 'state') {
      entry.unsentSince ??= change.committedAt;
      const wait = this.throttleMs - (this.now() - entry.lastEmit);
      if (wait <= 0 && !entry.flushTimer) this.#flushState(change.key);                    // quiet: right away
      else entry.flushTimer ??= setTimeout(() => this.#flushState(change.key), Math.max(0, wait)); // burst: end of the window
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
    // committedAt = the oldest commit not yet sent: measures the frame's worst delay.
    const committedAt = entry.unsentSince; entry.unsentSince = null; entry.lastEmit = this.now();
    this.#emit(key, { matchId: key, kind: 'STATE', version: entry.latest.version, state: entry.latest.state, committedAt });
  }

  #flush(key) {
    const entry = this.keys.get(key);
    if (!entry?.pending) return;
    // committedAt = the oldest commit in the window: the measured latency is that of the frame's oldest data.
    const { from, version, patch, committedAt } = entry.pending;
    entry.pending = null; entry.flushTimer = null; entry.delivered = version;
    // On flush the latest state is exactly at `version`.
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

// A subscriber's queue with lossless merging, which bounds memory without disconnecting:
//   a new STATE drops everything in the queue (the full state supersedes it all);
//   DELTA followed by DELTA becomes one DELTA (final-value patches merge);
//   CHECKPOINT with something in the queue is dropped (the queue already has newer info).
// Manual iterator so that return() releases the listener right away.
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
      // dropped

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
