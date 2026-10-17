import { setTimeout as delay } from 'node:timers/promises';
import { Synchronizer, runSync } from '../client/sync.mjs';
import { graphql, queryApi, transports } from '../client/transport.mjs';
import { startStack } from './stack.mjs';

// Latência commit → aplicação no cliente e custo do bootstrap.
//   MODE=edge: clientes em multipart pelo edge (dedup + cache), como atrás do Router
//   MODE=hive: clientes em graphql-ws direto no Hive Gateway (sem dedup nem cache)
// Os subgraphs rodam no mesmo processo dos clientes: números para comparar modos.
const clients = Number(process.env.CLIENTS ?? 50);
const rate = Number(process.env.RATE ?? 50);
const seconds = Number(process.env.SECONDS ?? 10);
const enrich = process.env.ENRICH === '1';
const mode = process.env.MODE ?? 'edge';
const stack = await startStack({ queueLimit: 10_000, checkpointMs: Number(process.env.CHECKPOINT_MS ?? 250) });
const url = mode === 'edge' ? stack.edge.url : stack.url;
const subscribe = transports[mode === 'edge' ? 'multipart' : 'ws'];
const scopeId = `bench-${Date.now()}`;
const write = (id, status) => graphql(url, 'mutation($s: ID!, $id: ID!, $v: String!) { upsertOrder(scopeId: $s, id: $id, status: $v) { cursor } }', { s: scopeId, id, v: status });

// Histórico antes dos clientes, com a base compartilhada aquecida no cursor 100:
// no modo edge, todos partem de uma base 400 eventos atrás e completam pelo log.
for (let i = 0; i < 100; i++) await write(String(i % 100), 'PENDING');
await queryApi(url, scopeId, { enrich }).snapshot('0');
for (let i = 100; i < 500; i++) await write(String(i % 100), 'PAID');
const latencies = []; const bootstrap = [];
const controller = new AbortController();
const started = Date.now();
const syncs = Array.from({ length: clients }, () => {
  const sync = new Synchronizer({ scopeId, api: queryApi(url, scopeId, { enrich }) });
  let first = true;
  const done = runSync({ sync, signal: controller.signal,
    connect: async function* (signal) {
      for await (const frame of subscribe(url, scopeId, { signal, enrich })) {
        if (frame.kind === 'CHANGE') latencies.push(Date.now() - frame.change.committedAt);
        yield frame;
      }
    },
    onUpdate: view => { if (first && view.synced) { first = false; bootstrap.push(Date.now() - started); } } });
  return { sync, done };
});
while (syncs.some(s => !s.sync.synced)) await delay(20);
const queriesAfterBootstrap = { ...stack.subgraphs.metrics };
latencies.length = 0;

const total = rate * seconds; const t0 = Date.now(); let head = 500;
for (let i = 0; i < total; i++) {
  const target = t0 + (i * 1000) / rate;
  if (target > Date.now()) await delay(target - Date.now());
  write(String(i % 100), ['PENDING', 'PAID', 'SHIPPED'][i % 3]).then(r => { head = Math.max(head, Number(r.upsertOrder.cursor)); });
}
const deadline = Date.now() + 15_000;
while (Date.now() < deadline && syncs.some(s => s.sync.cursor < 500 + total)) await delay(50);
controller.abort();
await Promise.all(syncs.map(s => s.done));
const m = stack.subgraphs.metrics; const edge = { ...stack.edge.metrics };
await stack.stop();

latencies.sort((a, b) => a - b); bootstrap.sort((a, b) => a - b);
const pct = (list, p) => list[Math.min(list.length - 1, Math.floor(list.length * p))];
console.log(JSON.stringify({
  mode, clients, rate, seconds, enrich,
  converged: syncs.filter(s => s.sync.cursor === 500 + total).length,
  liveLatencyMs: { p50: pct(latencies, 0.5), p95: pct(latencies, 0.95), p99: pct(latencies, 0.99), max: latencies.at(-1), n: latencies.length },
  bootstrapMs: { p50: pct(bootstrap, 0.5), max: bootstrap.at(-1) },
  subgraph: {
    liveStreamsOpened: m.liveStreamsOpened,
    snapshotRequests: queriesAfterBootstrap.snapshotRequests, changeRequests: queriesAfterBootstrap.changeRequests,
    queriesDuringLive: (m.snapshotRequests + m.changeRequests) - (queriesAfterBootstrap.snapshotRequests + queriesAfterBootstrap.changeRequests)
  },
  edge: mode === 'edge' ? { cacheHits: edge.cacheHits, cacheMisses: edge.cacheMisses, deduplicatedJoins: edge.deduplicatedJoins } : undefined
}, null, 2));
