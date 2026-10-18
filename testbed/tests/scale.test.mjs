import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { startStack } from '../emulator/stack.mjs';
import { startEdge } from '../emulator/edge.mjs';
import { createSync, runSync } from '../client/sync.mjs';
import { snapshotApi, transports } from '../client/transport.mjs';
import { scoreboardOps, applyStatEvents, playGenerator } from '../subgraphs/sports.mjs';

// Várias instâncias: 3 edges independentes (cada um com sua deduplicação e retenção,
// como instâncias do Router) atrás de um balanceador round-robin sem afinidade.
// Frame periódico de 2 s para que o efeito da retenção fique visível.
async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

let stack;
test.before(async () => {
  stack = await startStack({
    edgeCount: 3, edge: { identityHeaders: ['authorization'] },
    scoreboard: { periodicMs: 2000 }, stats: { periodicMs: 2000, throttleMs: 200 }
  });
});
test.after(async () => { await stack?.stop(); });

// Um cliente com um ou dois canais; registra todas as versões vistas para provar
// que nunca regridem, inclusive trocando de instância.
function client(url, matchId, channels, { headers = {} } = {}) {
  const controller = new AbortController();
  const c = { syncs: {}, seen: {}, errors: [], runs: [], failedAt: {}, frameAt: {} };
  for (const channel of channels) {
    const sync = createSync(channel, { matchId, api: snapshotApi(url, channel, matchId, { headers }) });
    c.syncs[channel] = sync; c.seen[channel] = [];
    c.runs.push(runSync({ sync, signal: controller.signal, wait: () => 100, stallMs: 5000,
      connect: signal => transports.multipart(url, channel, matchId, { signal, headers }),
      onUpdate: view => {
        if (view.version !== null) c.seen[channel].push(view.version);
        if (view.synced) c.frameAt[channel] = Date.now();
      },
      onError: error => { c.errors.push(error); c.failedAt[channel] = Date.now(); } }));
  }
  c.synced = () => Object.values(c.syncs).every(s => s.synced);
  // Every channel got a frame after its last error, i.e. it reconnected and resynced.
  c.recovered = () => c.synced() && channels.every(channel => !(c.failedAt[channel] >= c.frameAt[channel]));
  c.close = async () => { controller.abort(); await Promise.all(c.runs); };
  return c;
}
const monotonic = list => list.every((v, i) => i === 0 || v >= list[i - 1]);
const sum = (edges, op) => edges.reduce((n, e) => n + (e.metrics.upstreamByOperation[op] ?? 0), 0);

test('deduplicação por instância: upstreams = instâncias × partidas × canais, não clientes', { timeout: 60_000 }, async () => {
  const matches = ['scale-a', 'scale-b'];
  const clients = Array.from({ length: 30 }, (_, i) => client(stack.balancer.url, matches[i % 2], ['scoreboard', 'stats']));
  try {
    await until(() => clients.every(c => c.synced()), '30 clientes sincronizados');
    const perEdge = stack.edges.map(e => e.metrics.upstreamByOperation);
    console.log(JSON.stringify({ upstreamsPorInstancia: perEdge, streamsNoSubgraph: { placar: stack.subgraphs.metrics.scoreboard.activeStreams, estatisticas: stack.subgraphs.metrics.stats.activeStreams } }));
    for (const ops of perEdge) for (const op of ['Scoreboard', 'Stats']) assert.ok((ops[op] ?? 0) <= matches.length, 'no máximo 1 upstream por partida em cada instância');
    assert.ok(perEdge.filter(ops => Object.keys(ops).length).length >= 2, 'o balanceador espalhou os clientes');
    assert.equal(stack.subgraphs.metrics.scoreboard.activeStreams, sum(stack.edges, 'Scoreboard'));
    assert.equal(stack.subgraphs.metrics.stats.activeStreams, sum(stack.edges, 'Stats'));
    assert.ok(stack.subgraphs.metrics.stats.activeStreams <= stack.edges.length * matches.length);
  } finally { await Promise.all(clients.map(c => c.close())); }
});

test('retenção: late joiner num escopo parado recebe o estado na hora, sem esperar o periódico', { timeout: 60_000 }, async () => {
  const plain = await startEdge({ upstream: stack.url, retain: false, name: 'sem-retencao' });
  const measure = async (url, matchId) => {
    stack.subgraphs.stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
    const first = client(url, matchId, ['scoreboard']);
    await until(() => first.synced(), 'primeiro cliente');           // abre o upstream; recebe o frame inicial
    const t0 = Date.now();
    const late = client(url, matchId, ['scoreboard']);
    await until(() => late.synced(), 'late joiner', 5000);
    const ms = Date.now() - t0;
    await Promise.all([first.close(), late.close()]);
    return ms;
  };
  try {
    const retained = await measure(stack.edges[0].url, 'idle-retido');
    const notRetained = await measure(plain.url, 'idle-sem-retencao');
    console.log(JSON.stringify({ lateJoinerMs: { comRetencao: retained, semRetencao: notRetained, periodicoMs: 2000 } }));
    assert.ok(retained < 300, `com retenção: ${retained} ms`);
    assert.ok(notRetained > 1000, `sem retenção espera o periódico: ${notRetained} ms`);
    assert.ok(stack.edges[0].metrics.retainedReplays >= 1);
  } finally { await plain.close(); }
});

test('identidade na chave: tokens diferentes não compartilham upstream nem frame retido', { timeout: 60_000 }, async () => {
  const edge = stack.edges[0];
  const matchId = 'identidade';
  const before = { upstreams: edge.metrics.upstreamByOperation.Scoreboard ?? 0, replays: edge.metrics.retainedReplays };
  const a1 = client(edge.url, matchId, ['scoreboard'], { headers: { authorization: 'Bearer a' } });
  await until(() => a1.synced(), 'a1');
  const b = client(edge.url, matchId, ['scoreboard'], { headers: { authorization: 'Bearer b' } });
  const a2 = client(edge.url, matchId, ['scoreboard'], { headers: { authorization: 'Bearer a' } });
  try {
    await until(() => b.synced() && a2.synced(), 'b e a2');
    assert.equal((edge.metrics.upstreamByOperation.Scoreboard ?? 0) - before.upstreams, 2, 'um upstream por identidade');
    assert.equal(edge.metrics.retainedReplays - before.replays, 1, 'só a2 (mesma identidade de a1) recebeu o frame retido');
  } finally { await Promise.all([a1, b, a2].map(c => c.close())); }
});

test('instância cai no meio do jogo: clientes migram pelo balanceador, sem regressão de versão', { timeout: 90_000 }, async () => {
  const matchId = 'queda-instancia';
  const { stores } = stack.subgraphs;
  const next = playGenerator(matchId, 4);
  stores.scoreboard.update(matchId, s => scoreboardOps.startPeriod(s, { period: 1 }, Date.now()));
  const clients = Array.from({ length: 12 }, () => client(stack.balancer.url, matchId, ['scoreboard', 'stats']));
  let killedAt = 0; let recovery = null;
  try {
    await until(() => clients.every(c => c.synced()), '12 clientes sincronizados');
    const t0 = Date.now(); let goals = 0;
    while (Date.now() - t0 < 4000) {
      stores.stats.update(matchId, s => applyStatEvents(s, [next(), next()], matchId));
      if (Math.floor((Date.now() - t0) / 1000) > goals) { stores.scoreboard.update(matchId, s => scoreboardOps.scoreGoal(s, { side: 'HOME' }, Date.now())); goals++; }
      if (!killedAt && Date.now() - t0 > 1500) {
        // Time until every client is back in sync (reconnection + snapshot). Watched
        // from before the kill, since the clients can reconnect before killEdge returns;
        // the drop shows up as a client error, because `synced` only resets on
        // reconnection and the snapshot comes right after, so polling it can miss it.
        const errorsBefore = clients.reduce((n, c) => n + c.errors.length, 0);
        killedAt = Date.now();
        recovery = (async () => {
          await until(() => clients.reduce((n, c) => n + c.errors.length, 0) > errorsBefore, 'queda percebida', 3000);
          await until(() => clients.every(c => c.recovered()), 'recuperação', 10_000);
          return Date.now() - killedAt;
        })();
        recovery.catch(() => {}); // awaited below; no unhandled rejection meanwhile
        await stack.killEdge(1);
      }
      await delay(50);
    }
    const final = { scoreboard: stores.scoreboard.version(matchId), stats: stores.stats.version(matchId) };
    await until(() => clients.every(c => c.syncs.scoreboard.version === final.scoreboard && c.syncs.stats.version === final.stats), 'todos na versão final', 15_000);
    const migrated = clients.filter(c => c.errors.length);
    const recoveryMs = await recovery;
    console.log(JSON.stringify({ clientes: clients.length, migraram: migrated.length, recuperacaoMs: recoveryMs, upstreamsPorInstancia: stack.edges.map(e => e.metrics.upstreamByOperation), lb: stack.balancer.metrics }));
    assert.ok(migrated.length >= 1, 'algum cliente estava na instância derrubada');
    assert.ok(recoveryMs < 1500, `recuperação em ${recoveryMs} ms, sem esperar o periódico de 2 s`);
    for (const c of clients) for (const channel of ['scoreboard', 'stats']) assert.ok(monotonic(c.seen[channel]), `${channel} regrediu`);
    for (const c of clients) {
      assert.deepEqual(c.syncs.stats.state, stores.stats.read(matchId).state);
      assert.equal(c.syncs.scoreboard.state.home, stores.scoreboard.read(matchId).state.home);
    }
  } finally { await Promise.all(clients.map(c => c.close())); }
});

test('limpeza: sem assinantes, nenhuma instância guarda upstream ou frame retido', { timeout: 30_000 }, async () => {
  const alive = stack.edges.filter((_, i) => i !== 1);
  await until(() => alive.every(e => e.metrics.upstreamSubscriptions === 0 && e.metrics.retainedEntries === 0), 'edges limpos');
  await until(() => stack.subgraphs.metrics.scoreboard.activeStreams === 0 && stack.subgraphs.metrics.stats.activeStreams === 0, 'subgraph limpo');
});
