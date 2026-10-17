import { setTimeout as delay } from 'node:timers/promises';
import { StateSync, DeltaSync, runSync } from '../client/sync.mjs';
import { snapshotApi, transports } from '../client/transport.mjs';
import { scoreboardOps, applyStatEvents, playGenerator } from '../src/sports.mjs';
import { startStack } from './stack.mjs';

// Partida simulada com clientes reais pelo edge (dedup + cache, multipart como o Router):
//   SCORE_ONLY clientes de lista de jogos (só placar)
//   FULL       clientes da tela da partida (placar + estatísticas)
// Lances a EVENTS_PER_SECOND, com uma rajada de STORM_RATE lances/s no meio, e gols.
// Subgraphs e clientes no mesmo processo: números para comparar, não capacidade.
const scoreOnly = Number(process.env.SCORE_ONLY ?? 100);
const full = Number(process.env.FULL ?? 50);
const seconds = Number(process.env.SECONDS ?? 10);
const eventsPerSecond = Number(process.env.EVENTS_PER_SECOND ?? 20);
const stormRate = Number(process.env.STORM_RATE ?? 200);
const stack = await startStack({ stats: { coalesceMs: Number(process.env.COALESCE_MS ?? 500), checkpointMs: 1000 } });
const url = stack.edge.url;
const matchId = `bench-${Date.now()}`;
const next = playGenerator(matchId, 3);
const { stores } = stack.subgraphs;
stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
for (let i = 0; i < 400; i++) stores.stats.update(matchId, s => applyStatEvents(s, [next()], matchId)); // jogo já em andamento

const latency = { scoreboard: [], stats: [] };
const controller = new AbortController();
const clients = [];
const connect = (kind, channels) => {
  const client = { kind, bytes: { scoreboard: 0, stats: 0 }, syncs: {}, runs: [] };
  for (const channel of channels) {
    const sync = new (channel === 'scoreboard' ? StateSync : DeltaSync)({ matchId, api: snapshotApi(url, channel, matchId, { onBytes: n => { client.bytes[channel] += n; } }) });
    client.syncs[channel] = sync;
    client.runs.push(runSync({ sync, signal: controller.signal, connect: async function* (signal) {
      for await (const frame of transports.multipart(url, channel, matchId, { signal, onBytes: n => { client.bytes[channel] += n; } })) {
        if (frame.committedAt) latency[channel].push(Date.now() - frame.committedAt);
        yield frame;
      }
    } }));
  }
  clients.push(client);
};
for (let i = 0; i < scoreOnly; i++) connect('só placar', ['scoreboard']);
for (let i = 0; i < full; i++) connect('placar + estatísticas', ['scoreboard', 'stats']);
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
const after = stack.subgraphs.metrics; const edge = stack.edge.metrics;
controller.abort();
await Promise.all(clients.flatMap(c => c.runs));
await stack.stop();

const pct = (list, p) => { const s = [...list].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(s.length * p))]; };
const perClient = kind => {
  const group = clients.filter(c => c.kind === kind);
  if (!group.length) return undefined;
  const avg = channel => Math.round(group.reduce((n, c) => n + c.bytes[channel], 0) / group.length / elapsed);
  return { clientes: group.length, bytesPorSegundo: { placar: avg('scoreboard'), estatisticas: avg('stats') } };
};
console.log(JSON.stringify({
  duracaoS: +elapsed.toFixed(1), lances: plays, gols: goals, convergiram: `${converged}/${clients.length}`,
  latenciaMs: {
    placar: { p50: pct(latency.scoreboard, 0.5), p99: pct(latency.scoreboard, 0.99), max: pct(latency.scoreboard, 1), n: latency.scoreboard.length },
    estatisticas: { p50: pct(latency.stats, 0.5), p99: pct(latency.stats, 0.99), max: pct(latency.stats, 1), n: latency.stats.length }
  },
  porCliente: { 'só placar': perClient('só placar'), 'placar + estatísticas': perClient('placar + estatísticas') },
  subgraphs: {
    streamsAbertos: { placar: after.scoreboard.streamsOpened, estatisticas: after.stats.streamsOpened },
    framesEmitidos: { placar: after.scoreboard.framesEmitted - queriesBefore.scoreboard.framesEmitted, estatisticas: after.stats.framesEmitted - queriesBefore.stats.framesEmitted },
    snapshotsNoBootstrap: { placar: queriesBefore.scoreboard.snapshotRequests, estatisticas: queriesBefore.stats.snapshotRequests },
    snapshotsDuranteAoVivo: { placar: after.scoreboard.snapshotRequests - queriesBefore.scoreboard.snapshotRequests, estatisticas: after.stats.snapshotRequests - queriesBefore.stats.snapshotRequests }
  },
  edge: { upstreams: edge.upstreamByOperation, cacheHits: edge.cacheHits, cacheMisses: edge.cacheMisses }
}, null, 2));
