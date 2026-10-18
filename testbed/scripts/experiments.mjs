import { Store } from '../subgraphs/store.mjs';
import { ChannelHub } from '../subgraphs/channels.mjs';
import { initialScoreboard, initialStats, scoreboardOps, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';

// Design experiments with the real hubs and simulated time (the caller controls
// the clock, e.g. node:test's mock.timers). Generates a match and returns, for
// each policy, the frames a subscriber would get, with the instant and size
// in bytes in the delivered format (the Router's multipart: {"payload":{"data":...}}).
const statsOut = state => ({ ...state, players: Object.entries(state.players).map(([id, p]) => ({ id, ...p })) });
const wire = (field, frame) => JSON.stringify({ payload: { data: { [field]: frame } } }).length;

export const STATS_POLICIES = {
  'full state on every play': { coalesceMs: 0, full: true },
  'full state every 500 ms': { coalesceMs: 500, full: true },
  'patch on every play': { coalesceMs: 0, full: false, adaptive: false },
  'patch every 500 ms': { coalesceMs: 500, full: false, adaptive: false },
  'the smaller one per 500 ms window (adopted)': { coalesceMs: 500, full: false, adaptive: true }
};

// timeline: [{ at: ms, type: 'goal' | 'stats', count }] on top of the base pace.
export function simulateMatch({ tick, now, seconds = 60, eventsPerSecond = 20, storms = [], goals = [], stepMs = 50 }) {
  const matchId = 'm1';
  const out = { scoreboard: [], stats: Object.fromEntries(Object.keys(STATS_POLICIES).map(k => [k, []])), commits: { scoreboard: [], stats: [] } };
  const hubs = [];
  let scoreHub;
  const scores = new Store(':memory:', { initial: initialScoreboard, clock: now, onCommit: c => { out.commits.scoreboard.push(c); scoreHub.publish(c); } });
  scoreHub = new ChannelHub({ store: scores, mode: 'state', periodicMs: 1000 });
  const stats = new Store(':memory:', { initial: initialStats, clock: now, onCommit: c => { out.commits.stats.push(c); for (const h of hubs) h.publish(c); } });
  for (const [name, policy] of Object.entries(STATS_POLICIES)) {
    const hub = new ChannelHub({ store: stats, mode: 'delta', periodicMs: 1000, coalesceMs: policy.coalesceMs, adaptive: policy.adaptive ?? false });
    hubs.push(hub);
    hub.subscribe(matchId, frame => {
      // "Full state" policy: at the frame's instant the store is exactly at its version.
      const body = (policy.full && frame.kind === 'DELTA') || frame.kind === 'STATE'
        ? { matchId, kind: 'STATE', version: frame.version, committedAt: frame.committedAt, state: statsOut(stats.read(matchId).state) }
        : frame;
      out.stats[name].push({ t: now(), bytes: wire('statsUpdates', body), kind: frame.kind, committedAt: frame.committedAt });
    });
  }
  scoreHub.subscribe(matchId, frame => out.scoreboard.push({ t: now(), bytes: wire('scoreboardUpdates', frame), kind: frame.kind, committedAt: frame.committedAt, goal: frame.state.lastEvent?.type === 'GOAL' && frame.committedAt !== null }));

  const next = playGenerator(matchId, 11);
  scores.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, now()));
  const start = now();
  for (let t = 0; t < seconds * 1000; t += stepMs) {
    const storm = storms.find(s => t >= s.from && t < s.to);
    const perStep = ((storm ? storm.eventsPerSecond : eventsPerSecond) * stepMs) / 1000;
    for (let i = 0; i < perStep; i++) stats.update(matchId, s => applyStatEvents(s, [next()], matchId));
    for (const goal of goals) if (goal.at >= t && goal.at < t + stepMs) scores.update(matchId, s => scoreboardOps.scoreGoal(s, { side: goal.side }, now()));
    tick(stepMs);
  }
  tick(1000);
  const elapsed = (now() - start) / 1000;
  for (const h of [scoreHub, ...hubs]) for (const entry of h.keys.values()) { clearInterval(entry.timer); clearTimeout(entry.flushTimer); }
  scores.close(); stats.close();
  return { ...out, elapsed };
}

export const rate = (frames, elapsed) => ({ framesPerSecond: +(frames.length / elapsed).toFixed(1), bytesPerSecond: Math.round(frames.reduce((n, f) => n + f.bytes, 0) / elapsed) });

// FIFO link model with fixed bandwidth (e.g. a bad mobile connection): a frame only starts
// being transmitted when the previous one is done. Returns each frame's arrival instant.

export function throughLink(frames, bytesPerSecond) {
  let free = 0;
  return [...frames].sort((a, b) => a.t - b.t).map(frame => {
    const start = Math.max(frame.t, free);
    free = start + (frame.bytes / bytesPerSecond) * 1000;
    return { ...frame, arrival: free };
  });
}
