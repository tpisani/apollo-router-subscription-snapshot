import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { startStack } from '../scripts/stack.mjs';
import { verify } from '../scripts/verify.mjs';
import { Synchronizer, runSync } from '../client/sync.mjs';
import { queryApi, transports } from '../client/transport.mjs';

async function until(predicate, message, ms = 10_000) {
  for (const end = Date.now() + ms; Date.now() < end; await delay(10)) if (predicate()) return;
  throw new Error(`Timeout: ${message}`);
}

let stack;
test.before(async () => { stack = await startStack({ checkpointMs: 300, queueLimit: 50 }); });
test.after(async () => { await stack?.stop(); });

test('edge com deduplicação e cache de tudo: estado inicial consistente e ao vivo', { timeout: 60_000 }, async () => {
  const before = { ...stack.edge.metrics };
  const result = await verify({
    url: stack.edge.url, transport: 'multipart', metrics: async () => stack.subgraphs.metrics,
    prune: (scope, through) => stack.subgraphs.store.prune(scope, through)
  });
  console.log(JSON.stringify(result));
  assert.ok(stack.edge.metrics.deduplicatedJoins > before.deduplicatedJoins, 'houve entrada em stream deduplicado');
  assert.ok(stack.edge.metrics.cacheHits > before.cacheHits, 'houve respostas servidas do cache');
  await until(() => stack.subgraphs.metrics.activeLiveStreams === 0 && stack.subgraphs.metrics.hubListeners === 0, 'streams liberados');
});

test('mesmo protocolo direto no Hive Gateway (graphql-ws, sem dedup nem cache)', { timeout: 60_000 }, async () => {
  const result = await verify({ url: stack.url, transport: 'ws', enrich: true, expectDedup: false, expectStaleCache: false });
  assert.ok(result.head > 40);
});

test('reinício do subgraph com clientes no stream deduplicado', { timeout: 60_000 }, async () => {
  const scopeId = `restart-${Date.now()}`;
  const url = stack.edge.url;
  const controller = new AbortController();
  const syncs = [0, 1, 2].map(() => new Synchronizer({ scopeId, api: queryApi(url, scopeId) }));
  const runs = syncs.map(sync => runSync({ sync, signal: controller.signal, wait: () => 100,
    connect: signal => transports.multipart(url, scopeId, { signal }) }));
  try {
    stack.subgraphs.store.change(scopeId, '1', 'PENDING');
    await until(() => syncs.every(s => s.synced && s.cursor === 1), 'sincronizados');
    await stack.restartSubgraphs();
    stack.subgraphs.store.change(scopeId, '1', 'PAID'); stack.subgraphs.store.change(scopeId, '2', 'SHIPPED');
    await until(() => syncs.every(s => s.synced && s.cursor === 3), 'recuperados após reinício', 20_000);
    const truth = stack.subgraphs.store.snapshot(scopeId);
    for (const sync of syncs) assert.deepEqual(sync.view().orders, truth.orders);
  } finally { controller.abort(); await Promise.all(runs); }
});

test('rajada maior que a fila do stream: conflação em checkpoint, cliente recupera pelo histórico', { timeout: 60_000 }, async () => {
  const scopeId = `slow-${Date.now()}`;
  const url = stack.edge.url;
  const sync = new Synchronizer({ scopeId, api: queryApi(url, scopeId) });
  const controller = new AbortController();
  let stalls = 0;
  const conflationsBefore = stack.subgraphs.metrics.conflations;
  const run = runSync({ sync, signal: controller.signal, wait: () => 100,
    connect: async function* (signal) {
      for await (const frame of transports.multipart(url, scopeId, { signal })) {
        // Consumidor que trava de vez em quando; com fila pequena no subgraph,
        // não há perda para o cliente, só frames trocados por checkpoints.
        if (stalls++ % 5 === 0) await delay(50);
        yield frame;
      }
    } });
  try {
    await until(() => sync.synced, 'sincronizado');
    for (let i = 0; i < 300; i++) stack.subgraphs.store.change(scopeId, String(i % 9), ['PENDING', 'PAID', 'SHIPPED'][i % 3]);
    const truth = stack.subgraphs.store.snapshot(scopeId);
    await until(() => sync.cursor === Number(truth.cursor), 'alcançou o head');
    assert.deepEqual(sync.view().orders, truth.orders);
    assert.ok(stack.subgraphs.metrics.conflations > conflationsBefore, 'a fila do stream deveria ter transbordado');
    assert.ok(sync.stats.pages > 0, 'eventos descartados no stream vieram do histórico');
  } finally { controller.abort(); await run; }
});
