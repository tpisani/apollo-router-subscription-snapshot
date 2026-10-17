import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { startStack } from '../scripts/stack.mjs';
import { verify } from '../scripts/verify.mjs';
import { StateSync, DeltaSync, runSync } from '../client/sync.mjs';
import { snapshotApi, transports } from '../client/transport.mjs';
import { scoreboardOps, applyStatEvents, playGenerator } from '../src/sports.mjs';

async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

let stack;
test.before(async () => { stack = await startStack({ scoreboard: { checkpointMs: 300 }, stats: { checkpointMs: 300, coalesceMs: 200 } }); });
test.after(async () => { await stack?.stop(); });

test('edge com deduplicação e cache de tudo: placar e estatísticas em canais separados', { timeout: 60_000 }, async () => {
  const before = stack.edge.metrics;
  const result = await verify({ url: stack.edge.url, transport: 'multipart', metrics: async () => stack.subgraphs.metrics });
  console.log(JSON.stringify(result));
  const after = stack.edge.metrics;
  assert.ok(after.deduplicatedJoins > before.deduplicatedJoins, 'houve entrada em stream deduplicado');
  assert.ok(after.cacheHits > before.cacheHits, 'houve respostas servidas do cache');
  await until(() => ['scoreboard', 'stats'].every(c => stack.subgraphs.metrics[c].activeStreams === 0 && stack.subgraphs.metrics[c].hubListeners === 0), 'streams liberados');
});

test('mesmo protocolo direto no Hive Gateway (graphql-ws, sem dedup nem cache)', { timeout: 60_000 }, async () => {
  const result = await verify({ url: stack.url, transport: 'ws', expectDedup: false, expectStaleCache: false });
  assert.ok(result.versions.stats > 30, "cada lote de lances é um commit");
});

test('upstreams no edge: um por canal e partida, independente do número de clientes', { timeout: 60_000 }, async () => {
  const matchIds = ['dedup-1', 'dedup-2'];
  const controller = new AbortController();
  const runs = [];
  for (const matchId of matchIds) for (let i = 0; i < 5; i++) for (const channel of ['scoreboard', 'stats']) {
    const sync = new (channel === 'scoreboard' ? StateSync : DeltaSync)({ matchId, api: snapshotApi(stack.edge.url, channel, matchId) });
    runs.push({ sync, done: runSync({ sync, signal: controller.signal, connect: signal => transports.multipart(stack.edge.url, channel, matchId, { signal }) }) });
  }
  try {
    await until(() => runs.every(r => r.sync.synced), '20 clientes sincronizados');
    assert.deepEqual(stack.edge.metrics.upstreamByOperation, { Scoreboard: 2, Stats: 2 });
    assert.equal(stack.subgraphs.metrics.scoreboard.activeStreams, 2);
    assert.equal(stack.subgraphs.metrics.stats.activeStreams, 2);
  } finally { controller.abort(); await Promise.all(runs.map(r => r.done)); }
});

test('reinício dos subgraphs com clientes conectados: os dois canais se recuperam', { timeout: 60_000 }, async () => {
  const matchId = `restart-${Date.now()}`;
  const next = playGenerator(matchId, 9);
  const controller = new AbortController();
  const score = new StateSync({ matchId, api: snapshotApi(stack.edge.url, 'scoreboard', matchId) });
  const stats = new DeltaSync({ matchId, api: snapshotApi(stack.edge.url, 'stats', matchId) });
  const runs = [[score, 'scoreboard'], [stats, 'stats']].map(([sync, channel]) =>
    runSync({ sync, signal: controller.signal, wait: () => 100, connect: signal => transports.multipart(stack.edge.url, channel, matchId, { signal }) }));
  try {
    const stores = () => stack.subgraphs.stores;
    stores().scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
    stores().stats.update(matchId, s => applyStatEvents(s, [next(), next()], matchId));
    await until(() => score.synced && score.version === 1 && stats.synced && stats.version === 1, 'sincronizados');
    await stack.restartSubgraphs();
    stores().scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, Date.now()));
    for (let i = 0; i < 20; i++) stores().stats.update(matchId, s => applyStatEvents(s, [next()], matchId));
    await until(() => score.version === 2 && stats.version === 21 && score.synced && stats.synced, 'recuperados após reinício', 20_000);
    assert.equal(score.state.home, 1);
    assert.deepEqual(stats.state, stores().stats.read(matchId).state);
  } finally { controller.abort(); await Promise.all(runs); }
});
