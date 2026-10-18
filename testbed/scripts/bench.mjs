import { setTimeout as delay } from 'node:timers/promises';
import { createSync, runSync } from '../client/sync.mjs';
import { snapshotApi, transports } from '../client/transport.mjs';
import { scoreboardOps, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';
import { startStack } from '../emulator/stack.mjs';

// Simulated match with real clients through the edge (dedup + retention + cache, multipart like the Router);
// EDGES=3 puts 3 instances behind a round-robin balancer:
//   SCORE_ONLY match list clients (scoreboard only)
//   FULL       match screen clients (scoreboard + stats)
// Plays at EVENTS_PER_SECOND, with a burst of STORM_RATE plays/s in the middle, and goals.
// Subgraphs and clients in the same process: numbers for comparison, not capacity.
const scoreOnly = Number(process.env.SCORE_ONLY ?? 100);
const full = Number(process.env.FULL ?? 50);
const seconds = Number(process.env.SECONDS ?? 10);
const eventsPerSecond = Number(process.env.EVENTS_PER_SECOND ?? 20);
const stormRate = Number(process.env.STORM_RATE ?? 200);
const edgeCount = Number(process.env.EDGES ?? 1);
const stack = await startStack({ edgeCount, stats: { throttleMs: Number(process.env.THROTTLE_MS ?? 500), periodicMs: 1000 } });
const url = edgeCount > 1 ? stack.balancer.url : stack.edge.url;
const matchId = `bench-${Date.now()}`;
const next = playGenerator(matchId, 3);
const { stores } = stack.subgraphs;
stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
for (let i = 0; i < 400; i++) stores.stats.update(matchId, s => applyStatEvents(s, [next()], matchId)); // match already in progress

const latency = { scoreboard: [], stats: [] };
const bootstrap = []; // ms from opening each subscription to the first confirmed state
const controller = new AbortController();
const clients = [];
const connect = (kind, channels) => {
  const client = { kind, bytes: { scoreboard: 0, stats: 0 }, syncs: {}, runs: [] };
  for (const channel of channels) {
    const sync = createSync(channel, { matchId, api: snapshotApi(url, channel, matchId, { onBytes: n => { client.bytes[channel] += n; } }) });
    client.syncs[channel] = sync;
    const t0 = Date.now(); let measured = false;
    client.runs.push(runSync({ sync, signal: controller.signal,
      onUpdate: view => { if (view.synced && !measured) { measured = true; bootstrap.push(Date.now() - t0); } },
      connect: async function* (signal) {
      for await (const frame of transports.multipart(url, channel, matchId, { signal, onBytes: n => { client.bytes[channel] += n; } })) {
        if (frame.committedAt) latency[channel].push(Date.now() - frame.committedAt);
        yield frame;
      }
    } }));
  }
  clients.push(client);
};
for (let i = 0; i < scoreOnly; i++) connect('scoreboard only', ['scoreboard']);
for (let i = 0; i < full; i++) connect('scoreboard + stats', ['scoreboard', 'stats']);
while (clients.some(c => Object.values(c.syncs).some(s => !s.synced))) await delay(20);
latency.scoreboard.length = 0; latency.stats.length = 0;
for (const c of clients) c.bytes = { scoreboard: 0, stats: 0 };
const queriesBefore = stack.subgraphs.metrics;

const t0 = Date.now(); let goals = 0, plays = 0;
while (Date.now() - t0 < seconds * 1000) {
  const t = Date.now() - t0;
  const storm = t > seconds * 400 && t < seconds * 700;
  const perTick = Math.max(1, Math.round(((storm ? stormRate : eventsPerSecond) * 50) / 1000));
  for (let i = 0; i < perTick; i++) { stores.stats.update(matchId, s => applyStatEvents(s, [next()], matchId)); plays++; }
  if (Math.floor(t / 2000) > goals) { stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: goals % 2 ? 'HOME' : 'AWAY' }, Date.now())); goals++; }
  await delay(50);
}
const elapsed = (Date.now() - t0) / 1000;
await delay(1500);
const truth = { scoreboard: stores.scoreboard.read(matchId), stats: stores.stats.read(matchId) };
const converged = clients.filter(c => Object.entries(c.syncs).every(([ch, s]) => s.version === truth[ch].version)).length;
const after = stack.subgraphs.metrics; const edgeMetrics = stack.edges.map(e => e.metrics);
controller.abort();
await Promise.all(clients.flatMap(c => c.runs));
await stack.stop();

const pct = (list, p) => { const s = [...list].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(s.length * p))]; };
const perClient = kind => {
  const group = clients.filter(c => c.kind === kind);
  if (!group.length) return undefined;
  const avg = channel => Math.round(group.reduce((n, c) => n + c.bytes[channel], 0) / group.length / elapsed);
  return { clients: group.length, bytesPerSecond: { scoreboard: avg('scoreboard'), stats: avg('stats') } };
};
console.log(JSON.stringify({
  durationS: +elapsed.toFixed(1), plays, goals, converged: `${converged}/${clients.length}`,
  latencyMs: {
    scoreboard: { p50: pct(latency.scoreboard, 0.5), p99: pct(latency.scoreboard, 0.99), max: pct(latency.scoreboard, 1), n: latency.scoreboard.length },
    stats: { p50: pct(latency.stats, 0.5), p99: pct(latency.stats, 0.99), max: pct(latency.stats, 1), n: latency.stats.length },
    joinToSynced: { p50: pct(bootstrap, 0.5), max: pct(bootstrap, 1) }
  },
  perClient: { 'scoreboard only': perClient('scoreboard only'), 'scoreboard + stats': perClient('scoreboard + stats') },
  subgraphs: {
    streamsOpened: { scoreboard: after.scoreboard.streamsOpened, stats: after.stats.streamsOpened },
    framesEmitted: { scoreboard: after.scoreboard.framesEmitted - queriesBefore.scoreboard.framesEmitted, stats: after.stats.framesEmitted - queriesBefore.stats.framesEmitted },
    snapshotsAtBootstrap: { scoreboard: queriesBefore.scoreboard.snapshotRequests, stats: queriesBefore.stats.snapshotRequests },
    snapshotsWhileLive: { scoreboard: after.scoreboard.snapshotRequests - queriesBefore.scoreboard.snapshotRequests, stats: after.stats.snapshotRequests - queriesBefore.stats.snapshotRequests }
  },
  instances: edgeMetrics.map(e => ({ upstreams: e.upstreamByOperation, joinsWithRetainedFrame: e.retainedReplays, cacheHits: e.cacheHits }))

}, null, 2));
