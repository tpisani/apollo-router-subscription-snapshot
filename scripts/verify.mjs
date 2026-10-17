import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { Synchronizer, runSync } from '../client/sync.mjs';
import { graphql, queryApi, transports } from '../client/transport.mjs';

async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (await predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

// Cenário de ponta a ponta contra qualquer endpoint (edge emulado ou Apollo Router):
// cache antigo na base, late joiner num stream deduplicado, ao vivo sem queries,
// retomada sem snapshot, histórico expirado e rajada concorrente.
//   metrics(): métricas do subgraph orders (/metrics), para provar deduplicação.
//   prune(scope, through): opcional, só quando há acesso direto ao banco.
export async function verify({
  url, transport = 'multipart', metrics, prune, enrich = false,
  expectDedup = true, expectStaleCache = true, log = () => {}
}) {
  const scopeId = `verify-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
  const api = queryApi(url, scopeId, { enrich });
  const subscribe = transports[transport];
  const latencies = [];
  const clients = [];
  let lastCursor = 0;
  const write = async (id, status) => {
    const { upsertOrder } = await graphql(url, `mutation($s: ID!, $id: ID!, $v: String!) {
      upsertOrder(scopeId: $s, id: $id, status: $v) { cursor } }`, { s: scopeId, id, v: status });
    lastCursor = Math.max(lastCursor, Number(upsertOrder.cursor)); return upsertOrder.cursor;
  };
  const remove = async id => {
    const { deleteOrder } = await graphql(url, 'mutation($s: ID!, $id: ID!) { deleteOrder(scopeId: $s, id: $id) { cursor } }', { s: scopeId, id });
    lastCursor = Math.max(lastCursor, Number(deleteOrder.cursor));
  };
  const connect = (name, sync = new Synchronizer({ scopeId, api })) => {
    const controller = new AbortController();
    const errors = [];
    const done = runSync({ sync, signal: controller.signal, wait: () => 100,
      connect: async function* (signal) {
        for await (const frame of subscribe(url, scopeId, { signal, enrich })) {
          if (frame.kind === 'CHANGE') latencies.push(Date.now() - frame.change.committedAt);
          yield frame;
        }
      },
      onError: error => { errors.push(error); log(`${name}: reconectando (${error.message})`); } });
    const client = { name, sync, errors, close: async () => { controller.abort(); await done; } };
    clients.push(client); return client;
  };
  const synced = (client, cursor) => client.sync.synced && client.sync.cursor >= cursor;

  try {
    // 1. Aquece a chave compartilhada do snapshot e a deixa antiga.
    await write('1', 'PENDING');
    const warm = await api.snapshot('0');
    await write('1', 'PAID'); await write('2', 'PENDING');
    const stale = await api.snapshot('0');
    if (expectStaleCache) assert.equal(stale.cursor, warm.cursor, 'a base compartilhada deveria vir do cache, antiga');
    log(`base em cache: cursor ${stale.cursor}; head real ${lastCursor}`);

    // 2. Primeiro cliente: base antiga + histórico imutável até o alvo dado pelo stream.
    const a = connect('A');
    await until(() => synced(a, lastCursor), 'A sincronizado');
    assert.equal(a.sync.orders.get('1').status, 'PAID');
    assert.ok(a.sync.stats.pages >= 1, 'A completou a base antiga pelo histórico');

    // 3. Late joiner: entra num upstream já aberto, sem frame inicial próprio.
    const b = connect('B');
    await until(() => synced(b, lastCursor), 'B sincronizado (late joiner)');
    assert.deepEqual(b.sync.view(), a.sync.view());
    if (expectDedup && metrics) {
      await delay(300);
      const m = await metrics();
      assert.equal(m.activeLiveStreams, 1, `um único stream no subgraph para 2 clientes (veio ${m.activeLiveStreams})`);
    }

    // 4. Ao vivo: aplicado direto do frame, sem nenhuma query.
    const pagesBefore = a.sync.stats.pages + b.sync.stats.pages;
    await write('1', 'SHIPPED');
    await until(() => [a, b].every(c => c.sync.cursor === lastCursor), 'evento ao vivo');
    assert.equal(a.sync.stats.pages + b.sync.stats.pages, pagesBefore, 'evento ao vivo não deveria gerar query');

    // 5. Desconexão e retomada: mantém o estado, nenhum snapshot novo.
    await a.close();
    await write('2', 'PAID'); await remove('1');
    const snapshotsBefore = a.sync.stats.snapshots;
    const a2 = connect('A (retomado)', a.sync);
    await until(() => synced(a2, lastCursor), 'retomada de A');
    assert.equal(a.sync.stats.snapshots, snapshotsBefore, 'retomada não deveria buscar snapshot');
    assert.equal(a.sync.orders.has('1'), false);

    // 6. Histórico expirado: a base em cache (antiga) não alcança mais o histórico.
    if (prune) {
      prune(scopeId, lastCursor);
      await write('3', 'PENDING');
      const c = connect('C');
      await until(() => synced(c, lastCursor), 'C após expiração');
      assert.ok(c.sync.stats.reseeds >= 1, 'C deveria ter pedido snapshot com minCursor');
      assert.deepEqual(c.sync.view(), b.sync.view());
    }

    // 7. Rajada concorrente: todos convergem para o estado exato do banco.
    await Promise.all(Array.from({ length: 40 }, (_, i) => write(String(i % 6), ['PENDING', 'PAID', 'SHIPPED'][i % 3])));
    const truth = await api.snapshot(String(lastCursor)); // chave nova: não pode vir antiga
    assert.equal(truth.cursor, String(lastCursor));
    const live = clients.filter(c => c !== a);
    await until(() => live.every(c => synced(c, lastCursor)), 'convergência após rajada');
    for (const client of live) assert.deepEqual(client.sync.view().orders, truth.orders, `${client.name} difere do banco`);

    latencies.sort((x, y) => x - y);
    return {
      scopeId, head: lastCursor,
      latencyMs: { p50: latencies[Math.floor(latencies.length / 2)], p95: latencies[Math.floor(latencies.length * 0.95)], n: latencies.length },
      clients: Object.fromEntries(live.map(c => [c.name, c.sync.stats]))
    };
  } finally {
    await Promise.all(clients.map(c => c.close()));
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  // Contra um Apollo Router com licença (subscriptions + response cache), por exemplo
  // `docker compose --profile apollo up`. Use uma instância sem outros clientes.
  const url = process.env.GRAPHQL_URL ?? 'http://localhost:4000/graphql';
  const metricsUrl = process.env.METRICS_URL ?? 'http://localhost:4001/metrics';
  const result = await verify({
    url, transport: process.env.TRANSPORT ?? 'multipart',
    metrics: async () => (await fetch(metricsUrl)).json(),
    expectDedup: process.env.EXPECT_DEDUP !== '0', expectStaleCache: process.env.EXPECT_CACHE !== '0',
    log: console.log
  });
  console.log(JSON.stringify(result, null, 2));
  console.log('OK: base antiga em cache, late joiner deduplicado, ao vivo, retomada e convergência');
}
